{ lib, config, ... }:
{
  name = "fetch-git";

  imports = [
    ./testsupport/gitea.nix
  ];

  /*
  Test cases
  The following is set up automatically for each test case:
    - a repo with the {name} is created on the gitea server
    - a repo with the {name} is created on the client
    - the client repo is configured to push to the server repo
  Python variables:
    - repo.path: the path to the directory of the client repo
    - repo.git: the git command with the client repo as the working directory
    - repo.remote: the url to the server repo
  */
  testCases = [
    {
      name = "simple-http";
      description = "can fetch a git repo via http";
      script = ''
        # add a file to the repo
        client.succeed(f"""
          echo chiang-mai > {repo.path}/thailand \
          && {repo.git} add thailand \
          && {repo.git} commit -m 'commit1'
        """)

        # memoize the revision
        rev1 = client.succeed(f"""
          {repo.git} rev-parse HEAD
        """).strip()

        # push to the server
        client.succeed(f"""
          {repo.git} push origin main
        """)

        # fetch the repo via nix
        fetched1 = client.succeed(f"""
          nix eval --impure --raw --expr "(builtins.fetchGit {repo.remote}).outPath"
        """)

        # check if the committed file is there
        client.succeed(f"""
          test -f {fetched1}/thailand
        """)

        # check if the revision is the same
        rev1_fetched = client.succeed(f"""
          nix eval --impure --raw --expr "(builtins.fetchGit {repo.remote}).rev"
        """).strip()
        assert rev1 == rev1_fetched
      '';
    }
    {
      name = "respect-ref";
      description = "ensure fetched 'rev' is reachable from specified 'ref'";
      script = ''
        # make an initial commit
        client.succeed(f"""
          echo "init" > {repo.path}/init \
          && {repo.git} add init \
          && {repo.git} commit -m 'init'
        """)

        # push commit1 to branch1
        client.succeed(f"""
          echo chiang-mai > {repo.path}/thailand \
          && {repo.git} checkout -b branch1 \
          && {repo.git} add thailand \
          && {repo.git} commit -m 'commit1' \
          && {repo.git} push origin branch1
        """)

        # store rev of commit1
        commit1 = client.succeed(f"""
          {repo.git} rev-parse HEAD
        """).strip()

        # push commit2 to branch2
        client.succeed(f"""
          {repo.git} checkout main \
          && {repo.git} checkout -b branch2 \
          && echo bangkok > {repo.path}/thailand \
          && {repo.git} add thailand \
          && {repo.git} commit -m 'commit2' \
          && {repo.git} push origin branch2
        """)

        # try to fetch commit1 while specifying ref=branch2
        # this should fail
        client.fail(f"""
          nix eval --impure --raw --expr \'(builtins.fetchGit {{
            url = "{repo.remote}";
            rev = "{commit1}";
            ref = "branch2";
          }}).outPath'
        """)
      '';
    }
    {
      name = "respect-gitattributes";
      description = "ensure .gitattributes is respected";
      script = ''
        # add not-exported-file file to the repo
        client.succeed(f"""
          echo "not-exported-file" > {repo.path}/not-exported-file \
          && echo "not-exported-file export-ignore" >> {repo.path}/.gitattributes \
          && {repo.git} add not-exported-file .gitattributes \
          && {repo.git} commit -m 'commit1' \
          && {repo.git} push origin main
        """)

        # fetch the repo via nix
        fetched1 = client.succeed(f"""
          nix eval --impure --raw --expr "(builtins.fetchGit {repo.remote}).outPath"
        """)

        # ensure the file is not there
        client.fail(f"""
          test -f {fetched1}/not-exported-file
        """)
      '';
    }
  ];
}
